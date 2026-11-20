// Persistence, atomicity and physical-replication oracles for object statistics.
fn stats_test_directory() -> tempfile::TempDir {
    fs::create_dir_all("/tmp/mydb").unwrap();
    tempfile::tempdir_in("/tmp/mydb").unwrap()
}

fn object_stats_cold_descriptor_roundtrip(automatic: bool) {
    use crate::engine::{QueryResponse, StorageQueryEngine};

    let dir = stats_test_directory();
    let query = |engine: &StorageQueryEngine, sql: &str| {
        let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
            panic!("expected rows for {sql}");
        };
        result.rows
    };
    let definition = |table: &TableSchema| {
        let mut json = serde_json::to_value(table).unwrap();
        let object = json.as_object_mut().unwrap();
        object.remove("stats");
        object.remove("schema_version");
        json
    };
    let expected_schema;
    let expected_ddl;
    let expected_rows;
    {
        let engine = StorageQueryEngine::open(dir.path()).unwrap();
        engine.execute("CREATE TABLE t(id BIGINT UNSIGNED PRIMARY KEY, n DECIMAL(20,4), s VARCHAR(80) COLLATE utf8mb4_bin, b VARBINARY(16), d DATE, j JSON, g BIGINT AS (id+1) STORED, KEY ns(n,s), CONSTRAINT nonnegative CHECK(n>=0))").unwrap();
        engine.execute("INSERT INTO t(id,n,s,b,d,j) VALUES(1,12.3400,'hello',X'00FF','1969-12-31','{\"x\":1}'),(2,NULL,NULL,NULL,NULL,NULL)").unwrap();
        engine
            .execute("ALTER TABLE t ADD COLUMN tag VARCHAR(20) DEFAULT 'backfill'")
            .unwrap();
        let db = engine.database_for_replication();
        let before = db.snapshot_table_schema("test.t").unwrap();
        assert!(before.row_layouts.len() > 1);
        let persisted = db.decode_table_descriptor_rows(before.id).unwrap().unwrap();
        expected_schema = definition(&persisted);
        expected_ddl = query(&engine, "SHOW CREATE TABLE t");
        expected_rows = query(&engine, "SELECT * FROM t ORDER BY id");
        db.set_catalog_cache_mb(0);
        db.force_cold_online_descriptor_for_test("test.t");
        assert!(db.catalog_read().unwrap().tables["test.t"]
            .columns
            .is_empty());

        if automatic {
            assert_eq!(db.auto_analyze_stale_tables(0.1).unwrap(), 1);
            assert_eq!(db.auto_analyze_stale_tables(0.1).unwrap(), 0);
        } else {
            engine.execute("ANALYZE TABLE t").unwrap();
        }
        let persisted = db.decode_table_descriptor_rows(before.id).unwrap().unwrap();
        assert_eq!(definition(&persisted), expected_schema);
        assert_eq!(persisted.stats.row_count, 2);
        assert_eq!(persisted.stats.columns["n"].null_count, 1);
        assert_eq!(query(&engine, "SHOW CREATE TABLE t"), expected_ddl);
        assert_eq!(query(&engine, "SELECT * FROM t ORDER BY id"), expected_rows);
        db.maintain_zone_maps().unwrap();
    }
    let engine = StorageQueryEngine::open(dir.path()).unwrap();
    let table = engine.database().snapshot_table_schema("test.t").unwrap();
    assert_eq!(definition(&table), expected_schema);
    assert_eq!(table.stats.row_count, 2);
    assert_eq!(query(&engine, "SHOW CREATE TABLE t"), expected_ddl);
    assert_eq!(query(&engine, "SELECT * FROM t ORDER BY id"), expected_rows);
}

#[test]
fn object_stats_explicit_analyze_preserves_cold_descriptor() {
    object_stats_cold_descriptor_roundtrip(false);
}

#[test]
fn object_stats_auto_analyze_preserves_cold_descriptor() {
    object_stats_cold_descriptor_roundtrip(true);
}

#[test]
fn object_stats_rejects_incomplete_descriptors_before_persistence() {
    let dir = stats_test_directory();
    let db = open_relaxed_test_db(dir.path());
    create_checkpoint_test_table(&db);
    let before = db.snapshot_table_schema("test.t").unwrap();
    let persisted = db.decode_table_descriptor_rows(before.id).unwrap().unwrap();
    let expected = serde_json::to_value(&persisted).unwrap();
    let dictionaries = crate::storage::data_dictionary::dd_tables()
        .iter()
        .map(|table| (table.id, db.scan_dd_table(table.id).unwrap()))
        .collect::<Vec<_>>();
    for invalid in [CatalogManager::object_header(&before).as_ref().clone(), {
        let mut table = before.as_ref().clone();
        table.row_layouts.clear();
        table
    }] {
        let err = db
            .publish_stats_candidate(&before, invalid.clone())
            .unwrap_err();
        assert_eq!(err.code(), 1030);
        let err = db
            .publish_stats_candidate(&invalid, before.as_ref().clone())
            .unwrap_err();
        assert_eq!(err.code(), 1030);
        for (id, rows) in &dictionaries {
            assert_eq!(&db.scan_dd_table(*id).unwrap(), rows);
        }
        let persisted = db.decode_table_descriptor_rows(before.id).unwrap().unwrap();
        assert_eq!(serde_json::to_value(&persisted).unwrap(), expected);
    }
}

#[test]
fn object_stats_large_lob_roundtrip_and_commit_failure() {
    use crate::storage::{data_dictionary::DD_COLUMN_STATS, stats::ValueFrequency};
    let dir = stats_test_directory();
    let expected;
    {
        let db = open_relaxed_test_db(dir.path());
        create_checkpoint_test_table(&db);
        let txn = db.begin_txn().unwrap();
        db.insert_rows(txn, "test.t", vec![vec![Value::Int(1)]])
            .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        db.analyze_table("test.t").unwrap();
        let before = db.table("test.t").unwrap();
        let mut candidate = (*before).clone();
        let column = candidate.stats.columns.get_mut("id").unwrap();
        column.top_values = (0..180)
            .map(|i| ValueFrequency {
                value: Value::String(format!("{i:04}{}", "z".repeat(8192))),
                count: 1,
            })
            .collect();
        expected = serde_json::to_value(&candidate.stats).unwrap();
        let old = serde_json::to_value(&before.stats).unwrap();
        let modified = db.table_modification_delta(before.id);
        let live = db.table_live_row_delta(before.id);
        STATS_FAIL_AFTER_DD_ROWS.with(|v| v.set(true));
        assert!(db
            .publish_stats_candidate(&before, candidate.clone())
            .is_err());
        assert_eq!(
            serde_json::to_value(&db.table("test.t").unwrap().stats).unwrap(),
            old
        );
        assert_eq!(db.table_modification_delta(before.id), modified);
        assert_eq!(db.table_live_row_delta(before.id), live);
        let decoded = db.decode_table_descriptor_rows(before.id).unwrap().unwrap();
        assert_eq!(serde_json::to_value(&decoded.stats).unwrap(), old);
        db.publish_stats_candidate(&before, candidate).unwrap();
        let rows = db.scan_dd_table(DD_COLUMN_STATS).unwrap();
        assert!(rows
            .iter()
            .any(|r| matches!(&r[7],Value::Bytes(b) if b.len()>1024*1024)));
        let decoded = db.decode_table_descriptor_rows(before.id).unwrap().unwrap();
        assert_eq!(serde_json::to_value(&decoded.stats).unwrap(), expected);
        db.flush_dirty_data().unwrap();
    }
    let db = open_relaxed_test_db(dir.path());
    assert_eq!(
        serde_json::to_value(&db.table("test.t").unwrap().stats).unwrap(),
        expected
    );
}

#[test]
fn object_stats_explicit_histograms_are_typed_and_update_only_changed_objects() {
    use crate::engine::StorageQueryEngine;
    use crate::storage::data_dictionary::{DD_COLUMN_HISTOGRAMS, DD_COLUMN_STATS};
    let dir = stats_test_directory();
    let json;
    {
        let engine = StorageQueryEngine::open(dir.path()).unwrap();
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY, v DECIMAL(65,20), s VARCHAR(10000))")
            .unwrap();
        let big = "x".repeat(8192);
        // More than the retired explicit-histogram budget, using ordinary SQL values.
        for i in 0..80 {
            engine.execute(&format!("INSERT INTO t VALUES({i},123456789012345678901234567890123456789012345.12345678901234567890,'{i:04}{big}')")).unwrap();
        }
        engine.execute("ANALYZE TABLE t").unwrap();
        let db = engine.database_for_replication();
        let columns = db.scan_dd_table(DD_COLUMN_STATS).unwrap();
        engine
            .execute("ANALYZE TABLE t UPDATE HISTOGRAM ON v,s WITH 100 BUCKETS")
            .unwrap();
        let after = db.scan_dd_table(DD_COLUMN_STATS).unwrap();
        assert_eq!(
            columns, after,
            "histogram update rewrote unrelated column objects"
        );
        let table = db.table("test.t").unwrap();
        let histogram = &table.stats.explicit_histograms["v"];
        json = histogram.to_json();
        assert!(json.contains("123456789012345678901234567890123456789012345.12345678901234567890"));
        assert!(table.stats.explicit_histograms["s"].to_json().len() > 512 * 1024);
        assert_eq!(db.scan_dd_table(DD_COLUMN_HISTOGRAMS).unwrap().len(), 2);
        engine
            .execute("ANALYZE TABLE t DROP HISTOGRAM ON s")
            .unwrap();
        assert_eq!(db.scan_dd_table(DD_COLUMN_HISTOGRAMS).unwrap().len(), 1);
    }
    let engine = StorageQueryEngine::open(dir.path()).unwrap();
    assert_eq!(
        engine
            .database()
            .table("test.t")
            .unwrap()
            .stats
            .explicit_histograms["v"]
            .to_json(),
        json
    );
}

fn stats_copy_tree(source: &Path, target: &Path) {
    fs::create_dir_all(target).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let dest = target.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            stats_copy_tree(&entry.path(), &dest);
        } else {
            fs::copy(entry.path(), dest).unwrap();
        }
    }
}

#[test]
fn object_stats_native_replay_preserves_signed_baseline_and_restart() {
    use crate::engine::StorageQueryEngine;
    let master_dir = stats_test_directory();
    let replica_dir = stats_test_directory();
    let master = Arc::new(StorageQueryEngine::open(master_dir.path()).unwrap());
    master
        .execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    let initial = (0..100)
        .map(|i| format!("({i},{i})"))
        .collect::<Vec<_>>()
        .join(",");
    master
        .execute(&format!("INSERT INTO t VALUES {initial}"))
        .unwrap();
    master.execute("ANALYZE TABLE t").unwrap();
    let source = master.database_for_replication().clone();
    source.checkpoint().unwrap();
    stats_copy_tree(master_dir.path(), replica_dir.path());
    let mut role = crate::repl::role::RoleState::new_standalone();
    role.role = crate::repl::role::Role::Replica;
    crate::repl::role::save(replica_dir.path(), &role).unwrap();
    let mut start = source.wal_log().lock().next_lsn();
    for phase in 0..2 {
        let replica = StorageQueryEngine::open(replica_dir.path()).unwrap();
        let target = replica.database_for_replication().clone();
        if phase == 0 {
            let extra = (100..110)
                .map(|i| format!("({i},{i})"))
                .collect::<Vec<_>>()
                .join(",");
            master
                .execute(&format!("INSERT INTO t VALUES {extra}"))
                .unwrap();
            let hook = source.install_analyze_test_hook();
            let worker = source.clone();
            let analyze = thread::spawn(move || worker.analyze_table("test.t"));
            assert!(hook.wait_until_reached(Duration::from_secs(10)));
            master.execute("DELETE FROM t").unwrap();
            hook.release();
            analyze.join().unwrap().unwrap();
            master
                .execute("ANALYZE TABLE t UPDATE HISTOGRAM ON v WITH 10 BUCKETS")
                .unwrap();
        } else {
            master.execute("INSERT INTO t VALUES(999,999)").unwrap();
            master.execute("ANALYZE TABLE t").unwrap();
        }
        let log = source.wal_log();
        let safe = log.lock().next_lsn() - 1;
        crate::wal::LogManager::sync_durable_to(&log, safe).unwrap();
        let mut applier = crate::repl::replica::applier::Applier::with_database(
            replica_dir.path(),
            target.wal_log(),
            target.clone(),
            crate::repl::replica::progress::ReplicaProgress::default(),
        );
        let mut reader = crate::wal::WalReader::open(&log, start).unwrap();
        while let Some(chunk) = reader.next_chunk(safe, 32 * 1024).unwrap() {
            applier.apply_chunk(&chunk, &role.node_uuid, 1).unwrap();
        }
        let source_table = source.table("test.t").unwrap();
        let target_table = target.table("test.t").unwrap();
        assert_eq!(
            serde_json::to_value(&source_table.stats).unwrap(),
            serde_json::to_value(&target_table.stats).unwrap()
        );
        assert_eq!(
            source.table_live_row_delta(source_table.id),
            target.table_live_row_delta(target_table.id)
        );
        assert_eq!(
            source.table_modification_delta(source_table.id),
            target.table_modification_delta(target_table.id)
        );
        assert_eq!(
            target_table.stats.row_count as i128
                + target.table_live_row_delta(target_table.id) as i128,
            phase as i128
        );
        let last = target.wal_log().lock().next_lsn() - 1;
        crate::wal::LogManager::sync_durable_to(&target.wal_log(), last).unwrap();
        target
            .replica_restartpoint(Instant::now(), CHECKPOINT_TRIGGER_MANUAL)
            .unwrap();
        target.flush_dirty_data().unwrap();
        start = safe + 1;
    }
    let replica = StorageQueryEngine::open(replica_dir.path()).unwrap();
    assert_eq!(
        replica.database().table("test.t").unwrap().stats.row_count,
        1
    );
}

#[test]
#[ignore]
fn object_stats_crash_child() {
    let Some(path) = std::env::var_os("MYDB_STATS_CRASH_DIR") else {
        return;
    };
    let point = std::env::var("MYDB_STATS_CRASH_POINT")
        .unwrap()
        .parse::<u8>()
        .unwrap();
    let db = Database::open_with_durability(Path::new(&path), DurabilityMode::Strict).unwrap();
    let before = db.table("test.t").unwrap();
    let mut candidate = (*before).clone();
    candidate.stats.row_count = 12345;
    candidate.stats.columns.get_mut("id").unwrap().top_values = (0..180)
        .map(|i| crate::storage::ValueFrequency {
            value: Value::String(format!("{i:04}{}", "q".repeat(8192))),
            count: 1,
        })
        .collect();
    STATS_CRASH_POINT.with(|p| p.set(point));
    db.publish_stats_candidate(&before, candidate).unwrap();
    panic!("statistics crash point was not reached");
}

#[test]
fn object_stats_crash_recovery_is_atomic_before_and_after_commit() {
    for point in [1, 2] {
        let dir = stats_test_directory();
        {
            let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
            create_checkpoint_test_table(&db);
            let txn = db.begin_txn().unwrap();
            db.insert_rows(txn, "test.t", vec![vec![Value::Int(1)]])
                .unwrap();
            db.commit_txn(txn.txn_id).unwrap();
            db.analyze_table("test.t").unwrap();
        }
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "storage::core::tests::object_stats_crash_child",
            ])
            .env("MYDB_STATS_CRASH_DIR", dir.path())
            .env("MYDB_STATS_CRASH_POINT", point.to_string())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::inherit())
            .status()
            .unwrap();
        assert!(status.success());
        let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
        let table = db.table("test.t").unwrap();
        assert_eq!(table.stats.row_count, if point == 1 { 1 } else { 12345 });
        if point == 2 {
            assert_eq!(table.stats.columns["id"].top_values.len(), 180);
        }
    }
}

fn object_stats_publication_allows_reads_and_serializes_ddl(
    fail: bool,
    drop_table: bool,
    cold: bool,
) {
    use std::sync::mpsc;
    let dir = stats_test_directory();
    let db = Arc::new(open_relaxed_test_db(dir.path()));
    db.set_recyclebin_variable("mydb_recyclebin", 0).unwrap();
    create_checkpoint_test_table(&db);
    let before = db.table("test.t").unwrap();
    if cold {
        db.set_catalog_cache_mb(0);
        db.force_cold_online_descriptor_for_test("test.t");
    }
    let hook = Arc::new(DdlBackfillTestHook::new());
    let (published_tx, published_rx) = mpsc::channel();
    let worker = {
        let db = Arc::clone(&db);
        let hook = Arc::clone(&hook);
        let before = Arc::clone(&before);
        thread::spawn(move || {
            STATS_PUBLICATION_HOOK.with(|slot| *slot.borrow_mut() = Some(hook));
            STATS_FAIL_AFTER_DD_ROWS.with(|slot| slot.set(fail));
            let mut candidate = before.as_ref().clone();
            candidate.stats.analyze_revision += 1;
            published_tx
                .send(
                    db.publish_stats_candidate_checked(&before, candidate, true)
                        .map(|_| ()),
                )
                .unwrap();
        })
    };
    assert!(hook.wait_until_reached(Duration::from_secs(10)));
    // Never leave the worker paused when an assertion or timeout fails.
    struct Release(Arc<DdlBackfillTestHook>);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.release();
        }
    }
    let release = Release(Arc::clone(&hook));
    let (read_tx, read_rx) = mpsc::channel();
    let reader = {
        let db = Arc::clone(&db);
        let before = Arc::clone(&before);
        thread::spawn(move || {
            let table = db.table("test.t").unwrap();
            read_tx
                .send((
                    db.database_exists("test"),
                    table.schema_version,
                    table.stats.analyze_revision,
                    db.catalog.admission.try_read().is_some(),
                ))
                .unwrap();
            assert_eq!(table.id, before.id);
        })
    };
    // A publication reservation must coexist with normal table reads.
    let reader_owner = crate::storage::mdl::allocate_internal_owner().unwrap();
    let read_mdl = db.acquire_mdl(
        reader_owner,
        None,
        MdlDuration::Statement,
        vec![MdlRequest {
            key: MdlObjectKey::table("test", "t"),
            lock_type: MdlLockType::SharedRead,
        }],
        "test/read-during-statistics",
    );
    db.release_mdl_owner(reader_owner);
    let read = read_rx.recv_timeout(Duration::from_secs(2));
    let (ddl_tx, ddl_rx) = mpsc::channel();
    let ddl = {
        let db = Arc::clone(&db);
        thread::spawn(move || {
            let _definition = db.pin_ddl_table("test.t").unwrap();
            let result = if drop_table {
                db.drop_table("test.t")
            } else {
                db.set_column_comment("test.t", "id", "concurrent publication")
            };
            ddl_tx.send(result).unwrap();
        })
    };
    assert!(ddl_rx.recv_timeout(Duration::from_millis(100)).is_err());
    drop(release);
    read_mdl.unwrap();
    assert_eq!(
        read.unwrap(),
        (
            true,
            before.schema_version,
            before.stats.analyze_revision,
            true
        )
    );
    assert_eq!(
        published_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .is_err(),
        fail
    );
    ddl_rx
        .recv_timeout(Duration::from_secs(10))
        .unwrap()
        .unwrap();
    worker.join().unwrap();
    reader.join().unwrap();
    ddl.join().unwrap();
    if drop_table {
        assert!(db.table("test.t").is_err());
        assert!(db
            .decode_table_descriptor_rows(before.id)
            .unwrap()
            .is_none());
    } else {
        let table = db.table("test.t").unwrap();
        let persisted = db.decode_table_descriptor_rows(before.id).unwrap().unwrap();
        assert_eq!(table.columns[0].comment, "concurrent publication");
        assert_eq!(
            table.stats.analyze_revision,
            before.stats.analyze_revision + u64::from(!fail)
        );
        let live_rows = crate::storage::data_dictionary::dd_rows_for_table(&table).unwrap();
        let disk_rows = crate::storage::data_dictionary::dd_rows_for_table(&persisted).unwrap();
        assert_eq!(live_rows.per_table(), disk_rows.per_table());
    }
    drop(db);
    let db = open_relaxed_test_db(dir.path());
    assert_eq!(db.table("test.t").is_err(), drop_table);
    if !drop_table {
        let table = db.table("test.t").unwrap();
        assert_eq!(table.columns[0].comment, "concurrent publication");
        assert_eq!(
            table.stats.analyze_revision,
            before.stats.analyze_revision + u64::from(!fail)
        );
    }
}

#[test]
fn object_stats_durable_publication_does_not_block_reads_or_lose_concurrent_ddl() {
    object_stats_publication_allows_reads_and_serializes_ddl(false, false, false);
    object_stats_publication_allows_reads_and_serializes_ddl(false, false, true);
}

#[test]
fn object_stats_failed_publication_preserves_concurrent_ddl_and_persistent_revision() {
    object_stats_publication_allows_reads_and_serializes_ddl(true, false, false);
}

#[test]
fn object_stats_publication_does_not_resurrect_concurrently_dropped_table() {
    object_stats_publication_allows_reads_and_serializes_ddl(false, true, false);
}

#[test]
fn object_stats_cold_read_keeps_stable_revision_after_durable_commit_before_publication() {
    let dir = stats_test_directory();
    let db = Arc::new(
        Database::open_with_config(
            dir.path(),
            test_storage_config(DurabilityMode::Strict, true),
        )
        .unwrap(),
    );
    create_checkpoint_test_table(&db);
    let before = db.table("test.t").unwrap();
    db.set_catalog_cache_mb(0);
    db.force_cold_online_descriptor_for_test("test.t");
    let hook = Arc::new(DdlBackfillTestHook::new());
    let worker = {
        let db = db.clone();
        let hook = hook.clone();
        let before = before.clone();
        thread::spawn(move || {
            STATS_AFTER_COMMIT_HOOK.with(|slot| *slot.borrow_mut() = Some(hook));
            let mut candidate = before.as_ref().clone();
            candidate.stats.analyze_revision += 1;
            db.publish_stats_candidate(&before, candidate).unwrap();
        })
    };
    assert!(hook.wait_until_reached(Duration::from_secs(10)));
    let (tx, rx) = std::sync::mpsc::channel();
    let reader = {
        let db = db.clone();
        let id = before.id;
        thread::spawn(move || {
            tx.send((db.table("test.t"), db.decode_table_descriptor_rows(id)))
                .unwrap();
        })
    };
    let result = rx.recv_timeout(Duration::from_secs(2));
    hook.release();
    worker.join().unwrap();
    reader.join().unwrap();
    let (cold, durable) = result.unwrap();
    assert_eq!(cold.unwrap().schema_version, before.schema_version);
    assert_eq!(
        durable.unwrap().unwrap().stats.analyze_revision,
        before.stats.analyze_revision + 1
    );
    assert_eq!(
        db.table("test.t").unwrap().stats.analyze_revision,
        before.stats.analyze_revision + 1
    );
    drop(db);
    let reopened = Database::open_with_config(
        dir.path(),
        test_storage_config(DurabilityMode::Strict, true),
    )
    .unwrap();
    assert_eq!(
        reopened.table("test.t").unwrap().stats.analyze_revision,
        before.stats.analyze_revision + 1
    );
}

#[test]
fn object_stats_publication_keeps_unrelated_descriptor_cache_usable() {
    let dir = stats_test_directory();
    let db = Arc::new(open_relaxed_test_db(dir.path()));
    create_checkpoint_test_table(&db);
    db.create_table("u", vec![checkpoint_test_pk_column()], vec!["id".into()], vec![]).unwrap();
    db.set_catalog_cache_mb(64);
    let before = db.table("test.t").unwrap();
    db.force_cold_online_descriptor_for_test("test.u");
    let hook = Arc::new(DdlBackfillTestHook::new());
    let worker = {
        let db = db.clone();
        let hook = hook.clone();
        std::thread::spawn(move || {
            STATS_PUBLICATION_HOOK.with(|slot| *slot.borrow_mut() = Some(hook));
            let mut candidate = before.as_ref().clone();
            candidate.stats.analyze_revision += 1;
            db.publish_stats_candidate_checked(&before, candidate, true).unwrap();
        })
    };
    assert!(hook.wait_until_reached(Duration::from_secs(10)));
    let first = db.table("test.u").unwrap();
    let cached = !db.catalog_read().unwrap().tables["test.u"].columns.is_empty();
    let second = db.table("test.u").unwrap();
    let reused = Arc::ptr_eq(&first, &second);
    hook.release();
    worker.join().unwrap();
    assert!(cached && reused, "unrelated descriptor was decoded repeatedly during t's statistics I/O: cached={cached}, reused={reused}");
}

#[test]
fn object_stats_skips_pinned_ddl_and_rejects_stale_revision_before_dictionary_write() {
    let dir = stats_test_directory();
    let db = open_relaxed_test_db(dir.path());
    create_checkpoint_test_table(&db);
    let before = db.table("test.t").unwrap();
    let mut candidate = before.as_ref().clone();
    candidate.stats.analyze_revision += 1;
    let owner = crate::storage::mdl::allocate_internal_owner().unwrap();
    let ddl = StatisticsPublicationMdl(&db, owner);
    db.acquire_mdl(
        owner,
        None,
        MdlDuration::Statement,
        vec![MdlRequest {
            key: MdlObjectKey::table("test", "t"),
            lock_type: MdlLockType::SharedUpgradable,
        }],
        "test/ddl-before-statistics",
    )
    .unwrap();
    assert!(!db
        .publish_stats_candidate_checked(&before, candidate.clone(), true)
        .unwrap());
    drop(ddl);
    let lease = db.pin_ddl_table("test.t").unwrap();
    assert!(!db
        .publish_stats_candidate_checked(&before, candidate.clone(), true)
        .unwrap());
    drop(lease);
    db.set_column_comment("test.t", "id", "newer DDL").unwrap();
    let changed = db.decode_table_descriptor_rows(before.id).unwrap().unwrap();
    let err = db.publish_stats_candidate(&before, candidate).unwrap_err();
    assert!(err.is(crate::error::catalog::ER_TABLE_DEF_CHANGED));
    let persisted = db.decode_table_descriptor_rows(before.id).unwrap().unwrap();
    assert_eq!(
        serde_json::to_value(changed).unwrap(),
        serde_json::to_value(persisted).unwrap()
    );
    assert_eq!(db.table("test.t").unwrap().columns[0].comment, "newer DDL");
}
