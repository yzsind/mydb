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
