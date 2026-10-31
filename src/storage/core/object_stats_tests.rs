// Persistence, atomicity and physical-replication oracles for object statistics.
fn stats_test_directory() -> tempfile::TempDir {
    fs::create_dir_all("/tmp/mydb").unwrap();
    tempfile::tempdir_in("/tmp/mydb").unwrap()
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
